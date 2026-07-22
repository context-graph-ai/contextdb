use crate::formatter::{DEFAULT_ROW_CAP, format_query_result_capped, format_query_result_json};
use contextdb_core::{Error, TableMeta};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicy, SyncDirection};
use contextdb_server::{SyncClient, SyncPlugin};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::collections::HashMap;
use std::io::{BufRead, IsTerminal};
use std::sync::Arc;

/// Process exit code for a `.sync push` that was interrupted after its batch was
/// sent, where the hub could not confirm whether the batch landed. It is kept
/// DISTINCT from a clean success (0) and a definitive failure (1) so a scripted
/// `push && shutdown` never reads an unknown outcome as "landed" and closes the
/// machine on data that may never have reached the hub; re-pushing is safe
/// whether or not the batch committed. (clap uses 2; general errors 1; SIGINT
/// 130 — 3 is free.)
pub const EXIT_INTERRUPTED_PUSH_UNCONFIRMED: i32 = 3;

#[derive(Clone, Copy)]
struct InputContext {
    interactive: bool,
    script_line: Option<usize>,
    output: OutputOptions,
}

/// How query results are rendered. `--json` emits a JSON array of row objects
/// (uncapped); otherwise the human table is capped at [`DEFAULT_ROW_CAP`] rows
/// unless `all` (the `--all` flag) is set.
#[derive(Clone, Copy, Default)]
pub struct OutputOptions {
    pub json: bool,
    pub all: bool,
}

#[derive(Default)]
struct SessionState {
    had_error: bool,
    /// Set when a `.sync push` was interrupted after sending and the hub could
    /// not confirm the batch landed. Drives [`EXIT_INTERRUPTED_PUSH_UNCONFIRMED`]
    /// in non-interactive use; the interactive REPL prints the warning and
    /// continues.
    had_unconfirmed_push: bool,
    trace_enabled: bool,
}

/// Collapse a finished session's state to a process exit code: a definitive
/// error dominates (1), then an unconfirmed interrupted push (3), else success
/// (0).
fn session_exit_code(session: &SessionState) -> i32 {
    if session.had_error {
        1
    } else if session.had_unconfirmed_push {
        EXIT_INTERRUPTED_PUSH_UNCONFIRMED
    } else {
        0
    }
}

/// Run the REPL loop. Returns the process exit code: `0` if all commands
/// succeeded, `1` if any definitive error occurred, and
/// [`EXIT_INTERRUPTED_PUSH_UNCONFIRMED`] if a `.sync push` was interrupted after
/// sending with its outcome unconfirmed (and no harder error occurred).
pub fn run(
    db: Arc<Database>,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    sync_plugin: Option<&SyncPlugin>,
    output: OutputOptions,
) -> i32 {
    let interactive = std::io::stdin().is_terminal();
    if interactive {
        eprintln!("ContextDB v{}", env!("CARGO_PKG_VERSION"));
        eprintln!("Enter .help for usage hints.");
        return run_interactive(&db, sync_client, rt, sync_plugin, output);
    }

    run_scripted(&db, sync_client, rt, sync_plugin, output)
}

fn run_interactive(
    db: &Database,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    sync_plugin: Option<&SyncPlugin>,
    output: OutputOptions,
) -> i32 {
    let mut rl = DefaultEditor::new().expect("failed to initialize readline");
    let mut session = SessionState::default();

    loop {
        let readline = rl.readline("contextdb> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(line);
                if !process_input_line(
                    db,
                    sync_client,
                    rt,
                    line,
                    InputContext {
                        interactive: true,
                        script_line: None,
                        output,
                    },
                    sync_plugin,
                    &mut session,
                ) {
                    break;
                }
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => break,
            Err(_) => break,
        }
    }

    session_exit_code(&session)
}

fn run_scripted(
    db: &Database,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    sync_plugin: Option<&SyncPlugin>,
    output: OutputOptions,
) -> i32 {
    let mut session = SessionState::default();
    let stdin = std::io::stdin();
    for (line_idx, line) in stdin.lock().lines().enumerate() {
        let line = match line {
            Ok(line) => line,
            Err(_) => break,
        };
        let line = line.trim();
        if line.is_empty() || line.starts_with("--") {
            continue;
        }
        if !process_input_line(
            db,
            sync_client,
            rt,
            line,
            InputContext {
                interactive: false,
                script_line: Some(line_idx + 1),
                output,
            },
            sync_plugin,
            &mut session,
        ) {
            break;
        }
    }
    session_exit_code(&session)
}

fn process_input_line(
    db: &Database,
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    line: &str,
    input: InputContext,
    sync_plugin: Option<&SyncPlugin>,
    session: &mut SessionState,
) -> bool {
    if line.starts_with(".sync") || line.starts_with("\\sync") {
        let mut parts = line.splitn(2, ' ');
        let _cmd = parts.next();
        let rest = parts.next().unwrap_or("").trim();
        let outcome = run_sync_command(
            sync_client,
            rt,
            rest,
            sync_plugin,
            &mut session.had_unconfirmed_push,
        );
        println!("{}", outcome.message);
        if !outcome.ok {
            session.had_error = true;
        }
    } else if is_trace_command(line) {
        handle_trace_command(line, &mut session.trace_enabled);
    } else if is_explain_command(line) {
        if !handle_explain_command(db, line, input) {
            session.had_error = true;
        }
    } else if line.starts_with('.') || line.starts_with('\\') {
        if !handle_meta_command(db, sync_client, rt, line, sync_plugin) {
            return false;
        }
    } else {
        let upper = line.trim_start().to_uppercase();
        // The scripted INSERT echo is human affordance; under --json stdout is
        // the machine data channel, so suppress it to keep the stream clean.
        if !input.interactive && !input.output.json && upper.starts_with("INSERT") {
            println!("{line}");
        }
        if !execute_sql(db, line, input, session.trace_enabled) {
            session.had_error = true;
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
            if rest.eq_ignore_ascii_case("vector") {
                println!("VECTOR(N) WITH (quantization = 'F32'|'SQ8'|'SQ4')");
                println!("SHOW VECTOR_INDEXES");
                println!("ORDER BY embedding <=> [0.1, 0.2, 0.3] LIMIT 5");
                println!("ORDER BY embedding <=> ROW_VECTOR('table', 'embedding', $key) LIMIT 5");
                println!(
                    "Vector errors include LegacyVectorStoreDetected, StoreCorrupted, VectorIndexDimensionMismatch, UnknownVectorIndex, PersistedRowVectorRowMissing, and PersistedRowVectorCellNull"
                );
                return true;
            }
            if rest.eq_ignore_ascii_case("propagate") {
                println!(
                    "PROPAGATE ON EDGE <edge_type> INCOMING|OUTGOING|BOTH STATE <state> SET <state>"
                );
                println!("  [MAX DEPTH n] [ABORT ON FAILURE]");
                println!("PROPAGATE ON STATE <state> EXCLUDE VECTOR");
                println!(
                    "<column> REFERENCES <table>(<col>) ON STATE <state> PROPAGATE SET <state>"
                );
                println!();
                println!("Worked example:");
                println!("  CREATE TABLE decisions (");
                println!("    id UUID PRIMARY KEY,");
                println!("    status TEXT NOT NULL,");
                println!("    intention_id UUID REFERENCES intentions(id)");
                println!("      ON STATE archived PROPAGATE SET invalidated,");
                println!("    embedding VECTOR(384)");
                println!("  ) STATE MACHINE (status: active -> [invalidated, superseded])");
                println!("    PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated");
                println!("    PROPAGATE ON STATE invalidated EXCLUDE VECTOR");
                println!("    PROPAGATE ON STATE superseded EXCLUDE VECTOR");
                println!("See docs/query-language.md for the full grammar.");
                return true;
            }
            println!(".help / \\?          Show this message");
            println!(".help vector        Show vector index syntax and errors");
            println!(".help propagate     Show PROPAGATE DDL grammar and a worked example");
            println!(".quit/.exit / \\q    Exit REPL");
            println!(".tables / \\dt       List tables");
            println!(".schema / \\d <tbl>  Show table schema and constraints");
            println!(".explain <sql>      Show execution plan");
            println!(".trace on|off       Toggle one-line execution traces");
            println!(".sync status              Show sync connection info");
            println!(".sync push                Push local changes to server");
            println!(
                "                          exit codes: 0 = pushed; 1 = failed; 3 = interrupted after sending, outcome unconfirmed, re-push is safe"
            );
            println!(".sync pull                Pull remote changes from server");
            println!(".sync reconnect           Reconnect to the sync endpoint");
            println!(".sync destination         Move retained-data delivery to the connected hub");
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
                match explain_output(db, rest) {
                    Ok(plan) => print!("{}", plan),
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

fn is_trace_command(line: &str) -> bool {
    matches!(
        line.split_whitespace().next(),
        Some(".trace") | Some("\\trace")
    )
}

fn is_explain_command(line: &str) -> bool {
    matches!(line.split_whitespace().next(), Some(".explain"))
}

fn handle_trace_command(line: &str, trace_enabled: &mut bool) {
    let mut parts = line.split_whitespace();
    let _cmd = parts.next();
    match parts.next() {
        Some(state) if state.eq_ignore_ascii_case("on") => {
            *trace_enabled = true;
            println!("Trace enabled");
        }
        Some(state) if state.eq_ignore_ascii_case("off") => {
            *trace_enabled = false;
            println!("Trace disabled");
        }
        None => {
            let state = if *trace_enabled { "on" } else { "off" };
            println!("Trace: {state}");
        }
        Some(_) => println!("Usage: .trace on|off"),
    }
}

fn handle_explain_command(db: &Database, line: &str, input: InputContext) -> bool {
    let rest = line.strip_prefix(".explain").unwrap_or("").trim();
    if rest.is_empty() {
        eprintln!("Usage: .explain <sql>");
        return input.interactive;
    }

    match explain_output(db, rest) {
        Ok(plan) => {
            print!("{}", plan);
            true
        }
        Err(e) => {
            if is_fatal_cli_error(&e) {
                eprintln!("Error: {}", e);
            } else {
                println!("Error: {}", e);
            }
            input.interactive
        }
    }
}

fn explain_output(db: &Database, sql: &str) -> contextdb_core::Result<String> {
    if explain_can_use_runtime_trace(sql) {
        contextdb_engine::cli_render::render_explain(db, sql, &HashMap::new())
    } else {
        db.explain(sql).map(|mut plan| {
            if !plan.ends_with('\n') {
                plan.push('\n');
            }
            plan
        })
    }
}

fn explain_can_use_runtime_trace(sql: &str) -> bool {
    sql.split_whitespace().next().is_some_and(|token| {
        token.eq_ignore_ascii_case("SELECT") || token.eq_ignore_ascii_case("WITH")
    })
}

fn handle_sync_command(
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    args: &str,
    sync_plugin: Option<&SyncPlugin>,
) -> String {
    let mut unconfirmed_push = false;
    run_sync_command(sync_client, rt, args, sync_plugin, &mut unconfirmed_push).message
}

fn run_sync_command(
    sync_client: Option<&SyncClient>,
    rt: Option<&tokio::runtime::Runtime>,
    args: &str,
    sync_plugin: Option<&SyncPlugin>,
    unconfirmed_push: &mut bool,
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
            let base = crate::sync_status::render_sync_endpoint_status(
                &crate::sync_status::SyncEndpointStatusView {
                    tenant_id: client.tenant_id().to_string(),
                    endpoint: client.endpoint().to_string(),
                    transport_connected: connected,
                    database_lsn: client.db().current_lsn().to_string(),
                    push_watermark: client.push_watermark().to_string(),
                    pull_watermark: client.pull_watermark().to_string(),
                },
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
            Err(contextdb_core::Error::SyncPushUnconfirmed { .. }) => {
                // The batch reached the hub but the acknowledgement was lost, so
                // whether the data landed is genuinely unknown. Keep `ok` true so
                // this is NOT reported as a plain failure (usability job USR-19)
                // and the interactive REPL prints the warning and continues, but
                // flag it so non-interactive use exits with the distinct
                // EXIT_INTERRUPTED_PUSH_UNCONFIRMED code — a scripted
                // `push && shutdown` must not read this unknown outcome as a
                // clean success. Re-running `.sync push` reconciles idempotently
                // whether or not the batch committed.
                *unconfirmed_push = true;
                SyncCommandOutcome {
                    message: format!(
                        "Push interrupted after sending to {} (tenant {}); the hub did not acknowledge, so the data may or may not have landed. Run `.sync push` again to reconcile — it is safe whether or not the batch committed.",
                        client.endpoint(),
                        client.tenant_id()
                    ),
                    ok: true,
                }
            }
            Err(e) => SyncCommandOutcome {
                message: format!(
                    "Push to sync endpoint {} (tenant {}) failed: {e}. Check the endpoint is reachable, then retry with `.sync reconnect` followed by `.sync push`.",
                    client.endpoint(),
                    client.tenant_id()
                ),
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
                message: format!(
                    "Pull from sync endpoint {} (tenant {}) failed: {e}. Check the endpoint is reachable, then retry with `.sync reconnect` followed by `.sync pull`.",
                    client.endpoint(),
                    client.tenant_id()
                ),
                ok: false,
            },
        },
        "reconnect" => {
            rt.block_on(client.reconnect());
            let connected = rt.block_on(client.is_connected());
            let message = crate::sync_status::render_reconnect_outcome(connected);
            if connected {
                SyncCommandOutcome { message, ok: true }
            } else {
                SyncCommandOutcome { message, ok: false }
            }
        }
        "destination" => {
            // Move this edge's retained-data destination to the hub it is
            // currently, provably connected to. The operator points the process
            // at the new server (`--sync-endpoint`) and restarts, then runs
            // this; it authorises the move at the sync layer — clearing what the
            // PREVIOUS destination confirmed — so the next push rebuilds the new
            // destination from everything the edge holds, and nothing local is
            // deleted until the new destination confirms receipt.
            if rt.block_on(client.ensure_connected()).is_err() {
                return SyncCommandOutcome {
                    message: format!(
                        "Cannot change destination: the sync endpoint {} is unreachable. Start the CLI pointed at the new server with --sync-endpoint, then run `.sync destination`.",
                        client.endpoint()
                    ),
                    ok: false,
                };
            }
            let Some(node_id) = client.connected_hub_node_id() else {
                return SyncCommandOutcome {
                    message: "Cannot change destination: the transport does not authenticate a hub identity, so there is nothing to bind to. Use the dial-by-key (iroh) transport."
                        .to_string(),
                    ok: false,
                };
            };
            match client.change_destination(&node_id) {
                Ok(()) => SyncCommandOutcome {
                    message: format!(
                        "Retained-data destination is now hub {node_id}. Run `.sync push` to rebuild it from everything this edge holds; nothing local is deleted until it confirms receipt."
                    ),
                    ok: true,
                },
                Err(err) => SyncCommandOutcome {
                    message: format!("Changing destination failed: {err}"),
                    ok: false,
                },
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
            if let Err(err) = client.set_table_direction(table, dir) {
                // A delivery-promising table refuses a direction that would
                // stop delivering it; surface the engine's reason verbatim.
                return SyncCommandOutcome {
                    message: err.to_string(),
                    ok: false,
                };
            }
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
                    message: "Auto-sync is not enabled for this session. Restart the CLI with --tenant-id (and a sync endpoint) to turn it on.".to_string(),
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
                "Unknown sync command: {sub}. Try: status, push, pull, reconnect, destination, direction, policy, auto"
            ),
            ok: true,
        },
    }
}

fn print_table_meta(table: &str, meta: &TableMeta) {
    print!(
        "{}",
        contextdb_engine::cli_render::render_table_meta(table, meta)
    );
}

/// Execute a SQL statement and print the result. Returns `true` on success, `false` on error.
fn execute_sql(db: &Database, sql: &str, input: InputContext, trace_enabled: bool) -> bool {
    let output = input.output;
    match db.execute(sql, &HashMap::new()) {
        Ok(result) => {
            let rows_examined = db.__rows_examined();
            if result.columns.is_empty() {
                if output.json {
                    // A non-query statement: a small JSON status object so the
                    // machine channel stays JSON end to end.
                    println!(
                        "{}",
                        serde_json::json!({ "rows_affected": result.rows_affected })
                    );
                } else {
                    println!("ok (rows_affected={})", result.rows_affected);
                }
            } else if output.json {
                // Query result as a JSON array of row objects — uncapped.
                println!("{}", format_query_result_json(&result));
            } else {
                let show_empty_headers = sql
                    .trim()
                    .trim_end_matches(';')
                    .eq_ignore_ascii_case("SHOW VECTOR_INDEXES");
                let cap = if output.all {
                    None
                } else {
                    Some(DEFAULT_ROW_CAP)
                };
                let formatted = format_query_result_capped(&result, cap, show_empty_headers);
                println!("{formatted}");
            }
            if trace_enabled {
                println!(
                    "{}",
                    contextdb_engine::cli_render::render_query_trace(&result.trace, rows_examined)
                );
            }
            true
        }
        Err(e) => {
            let rendered = if matches!(e, Error::ParseError(_)) {
                if let Some(line) = input.script_line {
                    format!("line {line}: {e}")
                } else {
                    e.to_string()
                }
            } else {
                e.to_string()
            }
            .replace('\n', " ");
            let scripted_plan_error = !input.interactive && matches!(e, Error::PlanError(_));
            if scripted_plan_error || is_fatal_cli_error(&e) {
                eprintln!("Error: {rendered}");
                false
            } else if output.json {
                // Under --json, stdout is the machine data channel: a non-fatal
                // error must not print an "Error:" line there. Keep it on stderr
                // and continue (the session survives the recoverable error).
                eprintln!("Error: {rendered}");
                true
            } else {
                println!("Error: {rendered}");
                true
            }
        }
    }
}

pub fn is_fatal_cli_error_public(error: &Error) -> bool {
    is_fatal_cli_error(error)
}

fn is_fatal_cli_error(error: &Error) -> bool {
    match error {
        Error::ParseError(_)
        | Error::TableNotFound(_)
        | Error::NotFound(_)
        | Error::BfsDepthExceeded(_)
        | Error::RecursiveCteNotSupported
        | Error::WindowFunctionNotSupported
        | Error::FullTextSearchNotSupported
        | Error::VectorIndexDimensionMismatch { .. }
        | Error::UnknownVectorIndex { .. }
        | Error::PersistedRowVectorRowMissing { .. }
        | Error::PersistedRowVectorCellNull { .. }
        | Error::StoreCorrupted { .. }
        | Error::LegacyVectorStoreDetected { .. } => true,
        Error::MemoryBudgetExceeded { operation, .. } => operation.contains('@'),
        _ => false,
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
        let client = rt.block_on(async {
            SyncClient::new(
                db,
                "nats://localhost:19999",
                contextdb_core::TenantId::from("b3-test"),
            )
        });

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
        let client = rt.block_on(async {
            SyncClient::new(
                db,
                "nats://localhost:19999",
                contextdb_core::TenantId::from("b4-test"),
            )
        });

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
        let client = rt.block_on(async {
            SyncClient::new(
                db,
                "nats://localhost:19999",
                contextdb_core::TenantId::from("b5-test"),
            )
        });

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
        let rendered = contextdb_engine::cli_render::render_table_meta("repl_rt_sm", &meta);
        assert!(matches!(parse(&rendered), Ok(Statement::CreateTable(_))));
    }

    // USR-14: `.schema` must render the declared RETAIN and PROPAGATE policy —
    // both the table-level RETAIN / PROPAGATE ON EDGE / PROPAGATE ON STATE
    // EXCLUDE VECTOR clauses and the column-level foreign-key ON STATE PROPAGATE
    // SET form — and the rendered DDL must round-trip to an equivalent table.
    #[test]
    fn schema_render_includes_retain_and_propagate_round_trip() {
        let make_db = || {
            let db = Database::open_memory();
            db.execute(
                "CREATE TABLE intentions (id UUID PRIMARY KEY, status TEXT)",
                &HashMap::new(),
            )
            .expect("parent table must create");
            db
        };
        let decisions_ddl = "CREATE TABLE decisions (\
                id UUID PRIMARY KEY, \
                description TEXT NOT NULL, \
                status TEXT NOT NULL, \
                intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated, \
                embedding VECTOR(384)\
            ) STATE MACHINE (status: active -> [invalidated, superseded]) \
            RETAIN 30 DAYS SYNC SAFE \
            PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated \
            PROPAGATE ON STATE invalidated EXCLUDE VECTOR \
            PROPAGATE ON STATE superseded EXCLUDE VECTOR";

        let db = make_db();
        db.execute(decisions_ddl, &HashMap::new())
            .expect("decisions table must create");
        let meta = db.table_meta("decisions").expect("table meta");
        let rendered = contextdb_engine::cli_render::render_table_meta("decisions", &meta);

        // The declared policy must be visible in `.schema` output.
        assert!(
            rendered.contains("RETAIN 30 DAYS SYNC SAFE"),
            "rendered .schema must include the RETAIN clause, got:\n{rendered}"
        );
        assert!(
            rendered.contains("PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated"),
            "rendered .schema must include the edge PROPAGATE clause, got:\n{rendered}"
        );
        assert!(
            rendered.contains("PROPAGATE ON STATE invalidated EXCLUDE VECTOR"),
            "rendered .schema must include the vector-exclusion PROPAGATE clause, got:\n{rendered}"
        );
        assert!(
            rendered.contains("ON STATE archived PROPAGATE SET invalidated"),
            "rendered .schema must include the column foreign-key PROPAGATE form, got:\n{rendered}"
        );

        // Round-trip: the rendered DDL must re-parse and reconstruct an
        // equivalent table (re-rendering the reconstructed meta is identical).
        assert!(
            matches!(parse(&rendered), Ok(Statement::CreateTable(_))),
            "rendered .schema must re-parse as CREATE TABLE, got:\n{rendered}"
        );
        let db2 = make_db();
        db2.execute(&rendered, &HashMap::new()).unwrap_or_else(|e| {
            panic!("rendered DDL must execute on a fresh db: {e:?}\n{rendered}")
        });
        let meta2 = db2
            .table_meta("decisions")
            .expect("round-tripped table meta");
        let rerendered = contextdb_engine::cli_render::render_table_meta("decisions", &meta2);
        assert_eq!(
            rendered, rerendered,
            "re-rendering the round-tripped schema must be identical (equivalent TableMeta)"
        );
        assert_eq!(
            meta2.default_ttl_seconds,
            Some(30 * 24 * 60 * 60),
            "round-tripped RETAIN duration must survive"
        );
        assert!(meta2.sync_safe, "round-tripped SYNC SAFE flag must survive");
    }

    /// USR-19 exit contract. A `.sync push` interrupted after its batch was sent,
    /// where the hub cannot be reached to confirm the batch landed, must exit
    /// with the DISTINCT unconfirmed code (3) — never a plain success (0, which
    /// would let a `push && shutdown` caller close the machine on maybe-lost
    /// data) and never the definitive failure (1) that USR-19 removed. It still
    /// renders `ok` (the interactive REPL prints the warning and continues).
    #[test]
    fn interrupted_push_unconfirmed_maps_to_distinct_exit_code() {
        use contextdb_server::transport::{
            ClientTransport, TransportError, TransportFuture, TransportResult,
            TransportStatusFuture,
        };
        use std::time::Duration;

        // Connects, but every request after connect is unreachable — so the push
        // batch cannot be sent AND the reconcile status probe cannot confirm it.
        struct UnreachableAfterConnect;
        impl ClientTransport for UnreachableAfterConnect {
            fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
                Box::pin(async { Ok(()) })
            }
            fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
                Box::pin(async { true })
            }
            fn request<'a>(
                &'a self,
                _subject: &'a str,
                _bytes: Vec<u8>,
                _timeout: Duration,
            ) -> TransportFuture<'a, Vec<u8>> {
                Box::pin(async {
                    Err(TransportError::Unreachable(
                        "dial timed out (test)".to_string(),
                    ))
                })
            }
            fn request_single_reply<'a>(
                &'a self,
                _subject: &'a str,
                _bytes: Vec<u8>,
                _timeout: Duration,
            ) -> TransportFuture<'a, Vec<u8>> {
                Box::pin(async {
                    Err(TransportError::Unreachable(
                        "dial timed out (test)".to_string(),
                    ))
                })
            }
            fn ensure_single_reply_retry_safe(&self, _bytes: &[u8]) -> TransportResult<()> {
                Ok(())
            }
        }

        let db = Arc::new(Database::open_memory());
        db.execute(
            "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)",
            &HashMap::new(),
        )
        .unwrap();
        db.execute(
            "INSERT INTO notes (id, body) VALUES ('n1', 'pending')",
            &HashMap::new(),
        )
        .unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = SyncClient::with_transport(
            db,
            Arc::new(UnreachableAfterConnect),
            contextdb_core::TenantId::from("usr19-exit"),
        );

        // The library surfaces the distinct indeterminate error — never a bare
        // SyncError and never a false Ok.
        let push = rt.block_on(client.push());
        assert!(
            matches!(push, Err(Error::SyncPushUnconfirmed { .. })),
            "an unconfirmable interrupted push must be SyncPushUnconfirmed, got {push:?}"
        );

        // The CLI render keeps `ok` true (not a plain failure) but raises the
        // unconfirmed flag, so a non-interactive session exits with the distinct
        // code 3.
        let mut unconfirmed_push = false;
        let outcome = run_sync_command(
            Some(&client),
            Some(&rt),
            "push",
            None,
            &mut unconfirmed_push,
        );
        assert!(
            outcome.ok,
            "an unconfirmed push must not render as a plain failure"
        );
        assert!(
            unconfirmed_push,
            "an unconfirmed push must raise the distinct-exit flag"
        );
        assert!(
            outcome.message.contains("interrupted"),
            "the warning must name the interruption, got: {}",
            outcome.message
        );

        let session = SessionState {
            had_error: false,
            had_unconfirmed_push: unconfirmed_push,
            trace_enabled: false,
        };
        let code = session_exit_code(&session);
        assert_eq!(
            code, EXIT_INTERRUPTED_PUSH_UNCONFIRMED,
            "an unconfirmed push must map to the distinct exit code"
        );
        assert_ne!(code, 0, "must not be a clean-success exit");
        assert_ne!(code, 1, "must not be the definitive-failure exit");
        assert_eq!(EXIT_INTERRUPTED_PUSH_UNCONFIRMED, 3);
    }

    /// The exit-code ordering: a definitive error dominates (1), then an
    /// unconfirmed interrupted push (3), then a confirmed/clean run (0). The
    /// confirmed-landed push reports success at the library level in
    /// contextdb-server's `ip_interrupted_push_after_commit_*` test; here we pin
    /// how a clean session maps to exit 0.
    #[test]
    fn session_exit_code_orders_error_over_unconfirmed_over_success() {
        let clean = SessionState::default();
        assert_eq!(session_exit_code(&clean), 0, "a clean run exits 0");

        let unconfirmed = SessionState {
            had_error: false,
            had_unconfirmed_push: true,
            trace_enabled: false,
        };
        assert_eq!(
            session_exit_code(&unconfirmed),
            EXIT_INTERRUPTED_PUSH_UNCONFIRMED,
            "an unconfirmed push alone exits 3"
        );

        let errored = SessionState {
            had_error: true,
            had_unconfirmed_push: true,
            trace_enabled: false,
        };
        assert_eq!(
            session_exit_code(&errored),
            1,
            "a definitive error dominates an unconfirmed push"
        );
    }
}
