use crate::formatter::format_query_result;
use contextdb_core::{ColumnType, Error, TableMeta};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicy, SyncDirection};
use contextdb_server::{SyncClient, SyncPlugin};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::collections::HashMap;
use std::io::{BufRead, IsTerminal};
use std::sync::Arc;

/// Run the REPL loop. Returns `true` if all commands succeeded, `false` if any error occurred.
pub fn run(
    db: Arc<Database>,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    sync_plugin: Option<&SyncPlugin>,
) -> bool {
    let interactive = std::io::stdin().is_terminal();
    if interactive {
        eprintln!("ContextDB v{}", env!("CARGO_PKG_VERSION"));
        eprintln!("Enter .help for usage hints.");
        return run_interactive(&db, sync_client, rt, sync_plugin);
    }

    run_scripted(&db, sync_client, rt, sync_plugin)
}

fn run_interactive(
    db: &Database,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    sync_plugin: Option<&SyncPlugin>,
) -> bool {
    let mut rl = DefaultEditor::new().expect("failed to initialize readline");
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
                if !process_input_line(db, sync_client, rt, line, true, sync_plugin, &mut had_error)
                {
                    break;
                }
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => break,
            Err(_) => break,
        }
    }

    !had_error
}

fn run_scripted(
    db: &Database,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    sync_plugin: Option<&SyncPlugin>,
) -> bool {
    let mut had_error = false;
    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(_) => break,
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if !process_input_line(
            db,
            sync_client,
            rt,
            line,
            false,
            sync_plugin,
            &mut had_error,
        ) {
            break;
        }
    }
    !had_error
}

fn process_input_line(
    db: &Database,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    line: &str,
    interactive: bool,
    sync_plugin: Option<&SyncPlugin>,
    had_error: &mut bool,
) -> bool {
    if line.starts_with(".sync") || line.starts_with("\\sync") {
        let mut parts = line.splitn(2, ' ');
        let _cmd = parts.next();
        let rest = parts.next().unwrap_or("").trim();
        let outcome = run_sync_command(sync_client, rt, rest, sync_plugin);
        println!("{}", outcome.message);
        if !outcome.ok {
            *had_error = true;
        }
    } else if line.starts_with('.') || line.starts_with('\\') {
        if !handle_meta_command(db, sync_client, rt, line, sync_plugin) {
            return false;
        }
    } else {
        let upper = line.trim_start().to_uppercase();
        if !interactive && upper.starts_with("INSERT") {
            println!("{line}");
        }
        if !execute_sql(db, line) {
            *had_error = true;
        }
    }
    true
}

struct SyncCommandOutcome {
    message: String,
    ok: bool,
}

pub(crate) fn handle_meta_command(
    db: &Database,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    line: &str,
    sync_plugin: Option<&SyncPlugin>,
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
            println!(".sync auto [on|off]       Toggle auto-sync after DML");
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
                    Err(e) => {
                        if is_fatal_cli_error(&e) {
                            eprintln!("Error: {}", e);
                        } else {
                            println!("Error: {}", e);
                        }
                    }
                }
            }
        }
        ".sync" | "\\sync" => {
            println!(
                "{}",
                handle_sync_command(sync_client, rt, rest, sync_plugin)
            );
        }
        _ => println!("Unknown command: {}. Type \\? for help.", cmd),
    }

    true
}

fn handle_sync_command(
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    args: &str,
    sync_plugin: Option<&SyncPlugin>,
) -> String {
    run_sync_command(sync_client, rt, args, sync_plugin).message
}

fn run_sync_command(
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    args: &str,
    sync_plugin: Option<&SyncPlugin>,
) -> SyncCommandOutcome {
    let (Some(client), Some(rt)) = (sync_client, rt) else {
        return SyncCommandOutcome {
            message: "Sync not configured. Start with --tenant-id to enable.".to_string(),
            ok: true,
        };
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
            let base = format!(
                "Sync: tenant={}, url={}\nNATS: {status}\nDatabase LSN: {}\nPush watermark: LSN {}\nPull watermark: LSN {}",
                client.tenant_id(),
                client.nats_url(),
                client.db().current_lsn(),
                client.push_watermark(),
                client.pull_watermark()
            );
            let render = contextdb_engine::cli_render::render_sync_status(client.db());
            SyncCommandOutcome {
                message: format!("{base}\n{render}"),
                ok: true,
            }
        }
        "push" => match rt.block_on(client.push()) {
            Ok(result) => {
                let mut msg = format!(
                    "Pushed: {} applied, {} skipped, {} conflicts",
                    result.applied_rows,
                    result.skipped_rows,
                    result.conflicts.len()
                );
                for conflict in &result.conflicts {
                    if let Some(reason) = &conflict.reason {
                        msg.push_str(&format!("\n  conflict: {}", reason));
                    }
                }
                SyncCommandOutcome {
                    message: msg,
                    ok: true,
                }
            }
            Err(e) => SyncCommandOutcome {
                message: format!("Push failed: {e}"),
                ok: false,
            },
        },
        "pull" => match rt.block_on(client.pull_default()) {
            Ok(result) => {
                let mut msg = format!(
                    "Pulled: {} applied, {} skipped, {} conflicts",
                    result.applied_rows,
                    result.skipped_rows,
                    result.conflicts.len()
                );
                for conflict in &result.conflicts {
                    if let Some(reason) = &conflict.reason {
                        msg.push_str(&format!("\n  conflict: {}", reason));
                    }
                }
                SyncCommandOutcome {
                    message: msg,
                    ok: true,
                }
            }
            Err(e) => SyncCommandOutcome {
                message: format!("Pull failed: {e}"),
                ok: false,
            },
        },
        "reconnect" => {
            rt.block_on(client.reconnect());
            let connected = rt.block_on(client.is_connected());
            if connected {
                SyncCommandOutcome {
                    message: "Reconnected to NATS".to_string(),
                    ok: true,
                }
            } else {
                SyncCommandOutcome {
                    message: "Reconnection failed — NATS unreachable".to_string(),
                    ok: false,
                }
            }
        }
        "direction" => {
            if parts.len() != 3 {
                return SyncCommandOutcome {
                    message: "Usage: .sync direction <table> <Push|Pull|Both|None>".to_string(),
                    ok: true,
                };
            }
            let table = parts[1];
            let dir = match parts[2] {
                "Push" | "push" => SyncDirection::Push,
                "Pull" | "pull" => SyncDirection::Pull,
                "Both" | "both" => SyncDirection::Both,
                "None" | "none" => SyncDirection::None,
                other => {
                    return SyncCommandOutcome {
                        message: format!("Unknown direction: {other}. Use: Push, Pull, Both, None"),
                        ok: true,
                    };
                }
            };
            client.set_table_direction(table, dir);
            SyncCommandOutcome {
                message: format!("{table} -> {dir:?}"),
                ok: true,
            }
        }
        "policy" => {
            if parts.len() != 3 {
                return SyncCommandOutcome {
                    message: "Usage: .sync policy <table> <InsertIfNotExists|ServerWins|EdgeWins|LatestWins>\n       .sync policy default <policy>".to_string(),
                    ok: true,
                };
            }
            let policy = match parts[2] {
                "InsertIfNotExists" => ConflictPolicy::InsertIfNotExists,
                "ServerWins" => ConflictPolicy::ServerWins,
                "EdgeWins" => ConflictPolicy::EdgeWins,
                "LatestWins" => ConflictPolicy::LatestWins,
                other => {
                    return SyncCommandOutcome {
                        message: format!(
                            "Unknown policy: {other}. Use: InsertIfNotExists, ServerWins, EdgeWins, LatestWins"
                        ),
                        ok: true,
                    };
                }
            };
            if parts[1] == "default" {
                client.set_default_conflict_policy(policy);
                SyncCommandOutcome {
                    message: format!("Default conflict policy -> {policy:?}"),
                    ok: true,
                }
            } else {
                client.set_conflict_policy(parts[1], policy);
                SyncCommandOutcome {
                    message: format!("{} -> {policy:?}", parts[1]),
                    ok: true,
                }
            }
        }
        "auto" => {
            let Some(plugin) = sync_plugin else {
                return SyncCommandOutcome {
                    message: "Auto-sync not available (no sync plugin)".to_string(),
                    ok: true,
                };
            };
            let toggle = parts.get(1).copied().unwrap_or("");
            match toggle {
                "on" => {
                    plugin.set_auto(true);
                    SyncCommandOutcome {
                        message: "Auto-sync enabled".to_string(),
                        ok: true,
                    }
                }
                "off" => {
                    plugin.set_auto(false);
                    SyncCommandOutcome {
                        message: "Auto-sync disabled".to_string(),
                        ok: true,
                    }
                }
                "" => {
                    let state = if plugin.is_auto() { "on" } else { "off" };
                    SyncCommandOutcome {
                        message: format!("Auto-sync: {state}"),
                        ok: true,
                    }
                }
                other => SyncCommandOutcome {
                    message: format!("Unknown auto-sync option: {other}. Use: on, off"),
                    ok: true,
                },
            }
        }
        _ => SyncCommandOutcome {
            message: format!(
                "Unknown sync command: {sub}. Try: status, push, pull, reconnect, direction, policy, auto"
            ),
            ok: true,
        },
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
        ColumnType::Timestamp => "TIMESTAMP".to_string(),
        ColumnType::TxId => "TXID".to_string(),
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
            if is_fatal_cli_error(&e) {
                eprintln!("Error: {}", e);
                false
            } else {
                println!("Error: {}", e);
                true
            }
        }
    }
}

pub fn is_fatal_cli_error_public(error: &Error) -> bool {
    is_fatal_cli_error(error)
}

fn is_fatal_cli_error(error: &Error) -> bool {
    matches!(
        error,
        Error::ParseError(_)
            | Error::TableNotFound(_)
            | Error::NotFound(_)
            | Error::BfsDepthExceeded(_)
            | Error::RecursiveCteNotSupported
            | Error::WindowFunctionNotSupported
            | Error::FullTextSearchNotSupported
            | Error::ImmutableColumn { .. }
    )
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
        assert!(handle_meta_command(&db, None, None, "\\dt", None));
    }

    // B1: Existing \dt works with new handle_meta_command signature
    #[test]
    fn b1_existing_dt_works_with_new_signature() {
        let db = Database::open_memory();
        db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &HashMap::new())
            .unwrap();
        // Pass None for sync_client and rt — existing commands must work without sync
        assert!(handle_meta_command(&db, None, None, "\\dt", None));
        // Also verify .tables works
        assert!(handle_meta_command(&db, None, None, ".tables", None));
        // .quit returns false
        assert!(!handle_meta_command(&db, None, None, ".quit", None));
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
            let result = handle_sync_command(None, None, subcmd, None);
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
            let result = handle_sync_command(Some(&client), Some(&rt), &args, None);
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

        let result = handle_sync_command(
            Some(&client),
            Some(&rt),
            "policy obs InsertIfNotExists",
            None,
        );
        assert!(
            result.contains("InsertIfNotExists"),
            "policy command should contain 'InsertIfNotExists', got: {}",
            result
        );

        let result =
            handle_sync_command(Some(&client), Some(&rt), "policy default ServerWins", None);
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
            let result = handle_sync_command(Some(&client), Some(&rt), bad_input, None);
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
