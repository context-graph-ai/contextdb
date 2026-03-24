use crate::formatter::format_query_result;
use contextdb_core::{ColumnType, TableMeta};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicy, SyncDirection};
use contextdb_server::SyncClient;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::collections::HashMap;
use std::io::IsTerminal;
use std::sync::Arc;

/// Run the REPL loop. Returns `true` if all commands succeeded, `false` if any error occurred.
pub fn run(
    db: Arc<Database>,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
) -> bool {
    let mut rl = DefaultEditor::new().expect("failed to initialize readline");
    if std::io::stdin().is_terminal() {
        eprintln!("ContextDB v{}", env!("CARGO_PKG_VERSION"));
        eprintln!("Enter .help for usage hints.");
    }

    let mut had_error = false;

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
                    if !handle_meta_command(&db, sync_client, rt, line) {
                        break;
                    }
                } else if !execute_sql(&db, line) {
                    had_error = true;
                }
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => break,
            Err(_) => break,
        }
    }

    !had_error
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
            println!(".sync status              Show sync connection info");
            println!(".sync push                Push local changes to server");
            println!(".sync pull                Pull remote changes from server");
            println!(".sync reconnect           Reconnect to NATS");
            println!(".sync direction <t> <d>   Set table sync direction (Push|Pull|Both|None)");
            println!(
                ".sync policy <t> <p>      Set table conflict policy (InsertIfNotExists|ServerWins|EdgeWins|LatestWins)"
            );
            println!(".sync policy default <p>  Set default conflict policy");
        }
        ".tables" | "\\dt" => {
            for t in db.table_names() {
                println!("{t}");
            }
        }
        ".schema" | "\\d" => {
            if rest.is_empty() {
                eprintln!("Usage: .schema <table> or \\d <table>");
            } else if let Some(meta) = db.table_meta(rest) {
                print_table_meta(rest, &meta);
            } else {
                eprintln!("Table not found: {rest}");
            }
        }
        ".explain" => {
            if rest.is_empty() {
                eprintln!("Usage: .explain <sql>");
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
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    args: &str,
) -> String {
    let (Some(client), Some(rt)) = (sync_client, rt) else {
        return "Sync not configured. Start with --tenant-id to enable.".to_string();
    };

    let parts: Vec<&str> = args.split_whitespace().collect();
    let sub = parts.first().copied().unwrap_or("status");

    match sub {
        "status" => {
            let connected = rt.block_on(client.ensure_connected()).is_ok();
            let status = if connected {
                "connected"
            } else {
                "unreachable"
            };
            format!(
                "Sync: tenant={}, url={}\nNATS: {status}\nDatabase LSN: {}\nPush watermark: LSN {}\nPull watermark: LSN {}",
                client.tenant_id(),
                client.nats_url(),
                client.db().current_lsn(),
                client.push_watermark(),
                client.pull_watermark()
            )
        }
        "push" => match rt.block_on(client.push()) {
            Ok(result) => format!(
                "Pushed: {} applied, {} skipped, {} conflicts",
                result.applied_rows,
                result.skipped_rows,
                result.conflicts.len()
            ),
            Err(e) => format!("Push failed: {e}"),
        },
        "pull" => match rt.block_on(client.pull_default()) {
            Ok(result) => format!(
                "Pulled: {} applied, {} skipped, {} conflicts",
                result.applied_rows,
                result.skipped_rows,
                result.conflicts.len()
            ),
            Err(e) => format!("Pull failed: {e}"),
        },
        "reconnect" => {
            rt.block_on(client.reconnect());
            let connected = rt.block_on(client.is_connected());
            if connected {
                "Reconnected to NATS".to_string()
            } else {
                "Reconnection failed — NATS unreachable".to_string()
            }
        }
        "direction" => {
            if parts.len() != 3 {
                return "Usage: .sync direction <table> <Push|Pull|Both|None>".to_string();
            }
            let table = parts[1];
            let dir = match parts[2] {
                "Push" | "push" => SyncDirection::Push,
                "Pull" | "pull" => SyncDirection::Pull,
                "Both" | "both" => SyncDirection::Both,
                "None" | "none" => SyncDirection::None,
                other => {
                    return format!("Unknown direction: {other}. Use: Push, Pull, Both, None");
                }
            };
            client.set_table_direction(table, dir);
            format!("{table} -> {dir:?}")
        }
        "policy" => {
            if parts.len() != 3 {
                return "Usage: .sync policy <table> <InsertIfNotExists|ServerWins|EdgeWins|LatestWins>\n       .sync policy default <policy>".to_string();
            }
            let policy = match parts[2] {
                "InsertIfNotExists" => ConflictPolicy::InsertIfNotExists,
                "ServerWins" => ConflictPolicy::ServerWins,
                "EdgeWins" => ConflictPolicy::EdgeWins,
                "LatestWins" => ConflictPolicy::LatestWins,
                other => {
                    return format!(
                        "Unknown policy: {other}. Use: InsertIfNotExists, ServerWins, EdgeWins, LatestWins"
                    );
                }
            };
            if parts[1] == "default" {
                client.set_default_conflict_policy(policy);
                format!("Default conflict policy -> {policy:?}")
            } else {
                client.set_conflict_policy(parts[1], policy);
                format!("{} -> {policy:?}", parts[1])
            }
        }
        _ => format!(
            "Unknown sync command: {sub}. Try: status, push, pull, reconnect, direction, policy"
        ),
    }
}

fn print_table_meta(table: &str, meta: &TableMeta) {
    print!("{}", render_table_meta(table, meta));
}

fn render_table_meta(table: &str, meta: &TableMeta) -> String {
    let mut out = String::new();
    out.push_str(&format!("CREATE TABLE {table} (\n"));
    for (idx, col) in meta.columns.iter().enumerate() {
        let comma = if idx + 1 == meta.columns.len() {
            ""
        } else {
            ","
        };
        let nullable = if col.nullable { "" } else { " NOT NULL" };
        let pk = if col.primary_key { " PRIMARY KEY" } else { "" };
        out.push_str(&format!(
            "  {} {}{}{}{}",
            col.name,
            render_column_type(&col.column_type),
            nullable,
            pk,
            comma
        ));
        out.push('\n');
    }
    out.push_str(")\n");
    if meta.immutable {
        out.push_str("IMMUTABLE\n");
    }
    if let Some(sm) = &meta.state_machine {
        let mut entries: Vec<_> = sm.transitions.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        let transitions: Vec<String> = entries
            .into_iter()
            .map(|(from, tos)| format!("{from} -> [{}]", tos.join(", ")))
            .collect();
        out.push_str(&format!(
            "STATE MACHINE ({}: {})\n",
            sm.column,
            transitions.join(", ")
        ));
    }
    if !meta.dag_edge_types.is_empty() {
        let edge_types = meta
            .dag_edge_types
            .iter()
            .map(|edge_type| format!("'{edge_type}'"))
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!("DAG({edge_types})\n"));
    }
    out
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

/// Execute a SQL statement and print the result. Returns `true` on success, `false` on error.
fn execute_sql(db: &Database, sql: &str) -> bool {
    match db.execute(sql, &HashMap::new()) {
        Ok(result) => {
            if result.columns.is_empty() {
                println!("ok (rows_affected={})", result.rows_affected);
            } else {
                println!("{}", format_query_result(&result));
            }
            true
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contextdb_engine::sync_types::{ConflictPolicy, SyncDirection};
    use contextdb_parser::{Statement, parse};

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

    #[test]
    fn rt2_repl_schema_display_round_trip_parse() {
        let db = Database::open_memory();
        db.execute(
            "CREATE TABLE repl_rt_sm (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: pending -> [done])",
            &HashMap::new(),
        )
        .unwrap();

        let meta = db.table_meta("repl_rt_sm").expect("table meta");
        let rendered = render_table_meta("repl_rt_sm", &meta);
        assert!(matches!(parse(&rendered), Ok(Statement::CreateTable(_))));
    }
}
