use clap::Parser;
use std::io::IsTerminal;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

use contextdb_cli::{
    EXIT_ERROR, EXIT_INTERRUPTED_PUSH_UNCONFIRMED, EXIT_USAGE, ErrorClass, OutputOptions,
    auto_sync, run,
};
use contextdb_server::exit_codes::exit_code_for;

#[derive(Parser)]
#[command(
    name = "contextdb",
    version,
    after_help = "EXAMPLES:\n  \
        contextdb mydata.db\n    \
            Open (or create) a local database and start the interactive shell.\n\n  \
        echo \"SELECT * FROM decisions LIMIT 5\" | contextdb mydata.db\n    \
            Pipe one or more SQL statements in non-interactively and exit.\n\n  \
        contextdb :memory:\n    \
            Open a throwaway in-memory database.\n\n  \
        contextdb mydata.db --sync-endpoint <server-ticket> --tenant-id acme\n    \
            Open the database and connect to a contextdb-server using the enrollment\n    \
            ticket it printed (see `contextdb-server --help`).\n\n  \
        In the shell: .help propagate\n    \
            Show the PROPAGATE DDL grammar with a worked example."
)]
struct Args {
    /// Database path (:memory: for in-memory). Use `.help vector` for vector index syntax.
    path: String,

    /// Sync endpoint: the server's enrollment ticket (paste it verbatim).
    #[arg(long, env = "CONTEXTDB_SYNC_ENDPOINT")]
    sync_endpoint: Option<String>,

    /// Tenant ID for sync
    #[arg(long, env = "CONTEXTDB_TENANT_ID")]
    tenant_id: Option<String>,

    /// Memory limit (e.g. 4G, 512M). Sets startup ceiling.
    #[arg(long, env = "CONTEXTDB_MEMORY_LIMIT")]
    memory_limit: Option<String>,

    /// Disk limit for file-backed databases (e.g. 4G, 512M). Sets startup ceiling.
    #[arg(long, env = "CONTEXTDB_DISK_LIMIT")]
    disk_limit: Option<String>,

    /// Debounce interval for background auto-sync pushes.
    #[arg(long, env = "CONTEXTDB_SYNC_DEBOUNCE_MS", default_value_t = 500)]
    sync_debounce_ms: u64,

    /// Emit query results as a JSON array of row objects (uncapped) instead of a
    /// table, for scripts and agents. Non-query statements print a small JSON
    /// status object.
    #[arg(long)]
    json: bool,

    /// Print every row of a query result, disabling the default 100-row cap on
    /// table output.
    #[arg(long)]
    all: bool,
}

fn main() {
    let raw_args: Vec<String> = std::env::args().collect();
    if let Some(exit_code) = contextdb_cli::ops::dispatch_if_subcommand(&raw_args) {
        std::process::exit(exit_code);
    }
    let interactive = std::io::stdin().is_terminal();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    // Every message this binary writes — startup, shutdown, warnings — goes
    // through these, so a `--json` consumer parses one stderr format for the
    // whole process and not just for the REPL's own statements.
    let output = OutputOptions {
        json: args.json,
        all: args.all,
    };
    let memory_limit = args
        .memory_limit
        .as_ref()
        .map(|limit| parse_size_limit(limit))
        .transpose()
        .unwrap_or_else(|err| {
            // The invocation is wrong and nothing ran: a usage error, not a
            // failure of the run.
            output.report_error(ErrorClass::Usage, &format!("invalid --memory-limit: {err}"));
            std::process::exit(EXIT_USAGE);
        });
    let disk_limit = args
        .disk_limit
        .as_ref()
        .map(|limit| parse_size_limit(limit).map(|bytes| bytes as u64))
        .transpose()
        .unwrap_or_else(|err| {
            output.report_error(ErrorClass::Usage, &format!("invalid --disk-limit: {err}"));
            std::process::exit(EXIT_USAGE);
        });

    // If sync is configured, create the SyncPlugin before opening the DB.
    // Keep the rx end alive — a background task will consume it for debounced pushes.
    let (sync_plugin_arc, push_rx) = if args.tenant_id.is_some() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        (
            Some(Arc::new(contextdb_server::SyncPlugin::new(tx))),
            Some(rx),
        )
    } else {
        (None, None)
    };

    // Resolve the sync endpoint BEFORE the database is opened. An incomplete
    // flag combination is a usage error, and exit 2 promises that nothing was
    // attempted — which has to include not creating a database file on a fresh
    // path for a command that was never going to run.
    let sync_endpoint = args.tenant_id.as_ref().map(|_| {
        let endpoint = args.sync_endpoint.clone().unwrap_or_else(|| {
            output.report_error(
                ErrorClass::Usage,
                "--tenant-id needs a sync endpoint. Pass --sync-endpoint with the server's ticket.",
            );
            std::process::exit(EXIT_USAGE);
        });
        // A bare ticket gets the edge's own fabric identity pinned to it
        // (derived from the database path), so this machine dials as its
        // enrolled identity instead of an ephemeral key. In-memory databases
        // have no data root and stay ephemeral.
        use contextdb_server::transport::iroh::EndpointSpec;
        match EndpointSpec::parse(&endpoint) {
            Some(spec)
                if spec.dial_ticket().is_some()
                    && spec.identity_path().is_none()
                    && args.path != ":memory:" =>
            {
                format!(
                    "iroh:?to={}&identity={}.fabric-identity.key",
                    spec.dial_ticket().expect("checked above"),
                    args.path
                )
            }
            _ => endpoint,
        }
    });

    debug!(path = %args.path, "opening database");
    let opened = if args.path == ":memory:" {
        if let Some(ref plugin) = sync_plugin_arc {
            contextdb_engine::database::open_memory_with_startup_limit(plugin.clone(), memory_limit)
        } else {
            contextdb_engine::database::open_memory_with_startup_limit(
                Arc::new(contextdb_engine::plugin::CorePlugin),
                memory_limit,
            )
        }
    } else if let Some(ref plugin) = sync_plugin_arc {
        contextdb_engine::database::open_with_startup_limits(
            std::path::Path::new(&args.path),
            plugin.clone(),
            memory_limit,
            disk_limit,
        )
    } else {
        contextdb_engine::database::open_with_startup_limits(
            std::path::Path::new(&args.path),
            Arc::new(contextdb_engine::plugin::CorePlugin),
            memory_limit,
            disk_limit,
        )
    };
    let db = match opened {
        Ok(db) => db,
        Err(e) => {
            output.report_error(
                ErrorClass::of(&e),
                &format!("failed to open database at '{}': {e}", args.path),
            );
            std::process::exit(EXIT_ERROR);
        }
    };

    let db = Arc::new(db);

    // Single tokio runtime for the session.
    let rt_and_client = args.tenant_id.as_ref().map(|tenant_id| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("failed to create tokio runtime");
        let endpoint = sync_endpoint
            .clone()
            .expect("an endpoint is resolved whenever --tenant-id is present");
        let client = Arc::new(contextdb_server::SyncClient::new(
            db.clone(),
            &endpoint,
            contextdb_core::TenantId::from(tenant_id.as_str()),
        ));
        (rt, client)
    });

    let (rt, sync_client) = match &rt_and_client {
        Some((rt, client)) => (Some(rt), Some(client)),
        None => (None, None),
    };

    let probe_handle = if !interactive && let (Some(rt), Some(client)) = (rt, sync_client) {
        // Warm the sync connection in the background. Never wrap this in a
        // timeout that abandons the future: dropping a mid-dial endpoint
        // aborts it ungracefully (a scary transport ERROR on stderr). A down
        // endpoint surfaces as one clear warning instead of silence. The
        // handle is settled before shutdown so a late-finishing probe can
        // never store an endpoint after the state was drained.
        let probe = Arc::clone(client);
        Some(rt.spawn(async move {
            if let Err(err) = probe.ensure_connected().await {
                // Not a failure: the session runs locally and sync retries.
                output.report_notice(
                    ErrorClass::Sync,
                    &format!("Warning: sync endpoint unreachable: {err}"),
                );
            }
        }))
    } else {
        None
    };

    // Spawn background debounced push task if sync is configured.
    let push_handle = if let (Some(rt_ref), Some(client), Some(rx)) = (rt, sync_client, push_rx) {
        let client_clone = Arc::clone(client);
        let plugin_clone = sync_plugin_arc.clone().expect("sync plugin configured");
        let config = auto_sync::AutoSyncConfig {
            debounce: Duration::from_millis(args.sync_debounce_ms),
            ..auto_sync::AutoSyncConfig::default()
        };
        Some(rt_ref.spawn(auto_sync::run_loop(
            rx,
            config,
            move || {
                let client = client_clone.clone();
                let plugin = plugin_clone.clone();
                async move {
                    let result = match client.push().await {
                        Ok(result) => result,
                        Err(contextdb_core::Error::SyncPushUnconfirmed { detail }) => {
                            // Interrupted after the batch was sent: the data may
                            // already have landed on the hub. This is NOT a
                            // failure — auto-sync will re-push, which reconciles
                            // idempotently whether or not the batch committed.
                            output.report_notice(
                                ErrorClass::Sync,
                                &format!("Background auto-sync push was interrupted before the hub acknowledged: {detail}. It will re-push to reconcile."),
                            );
                            return Ok(auto_sync::PushOutcome {
                                conflicts: Vec::new(),
                                caught_up: false,
                            });
                        }
                        Err(contextdb_core::Error::SyncReplayOfAcceptedDelete { table, key }) => {
                            // The store already agrees: benign convergence,
                            // not a failure -- see
                            // `Error::SyncReplayOfAcceptedDelete`'s doc
                            // comment. Report it once as a notice (not a
                            // repeating "could not push" failure message)
                            // and mark caught up so auto-sync stops
                            // retrying a push that will never resolve any
                            // differently.
                            output.report_notice(
                                ErrorClass::Sync,
                                &format!(
                                    "Background auto-sync push re-offered {table} {key:?}, which \
                                     the hub already converged on as deleted; nothing to do."
                                ),
                            );
                            return Ok(auto_sync::PushOutcome {
                                conflicts: Vec::new(),
                                caught_up: true,
                            });
                        }
                        Err(err) => return Err(err.to_string()),
                    };
                    Ok(auto_sync::PushOutcome {
                        conflicts: result
                            .conflicts
                            .iter()
                            .map(contextdb_engine::cli_render::sync_conflict_document)
                            .collect::<Vec<_>>(),
                        caught_up: client.push_watermark() >= plugin.pending_lsn(),
                    })
                }
            },
            // The worker keeps retrying, so its report is a notice, not a
            // failure of this run.
            move |report| match report {
                auto_sync::AutoSyncReport::Conflict(conflict) => output
                    .report_notice_document(ErrorClass::Sync, "sync conflict", &conflict),
                auto_sync::AutoSyncReport::Message(message) => {
                    output.report_notice(ErrorClass::Sync, &message)
                }
            },
        )))
    } else {
        None
    };

    // Process exit code, from the table in `contextdb_server::exit_codes`
    // (documented at docs/cli.md, "Exit Codes"): 0 = clean, 1 = definitive
    // error, 3 = an interrupted push whose outcome the hub could not confirm.
    // A definitive error always dominates an unconfirmed push; an unconfirmed
    // push never downgrades a 1.
    let mut exit_code = run(
        db.clone(),
        sync_client.map(|c| c.as_ref()),
        rt,
        sync_plugin_arc.as_deref(),
        output,
    );

    // Graceful shutdown: stop background notifications, wait for any in-flight
    // auto-sync work to finish, then do one final flush before closing the DB.
    if let Some((rt, client)) = rt_and_client {
        if let Err(err) = db.execute("ROLLBACK", &std::collections::HashMap::new()) {
            output.report_error(
                ErrorClass::of(&err),
                &format!("Final transaction rollback failed: {err}"),
            );
            exit_code = EXIT_ERROR;
        }
        if let Some(ref plugin) = sync_plugin_arc {
            plugin.shutdown();
        }
        if let Some(handle) = push_handle
            && let Err(err) = rt.block_on(handle)
        {
            // A join failure of the auto-sync worker is a failure of the sync
            // plane, not of the statements this session ran.
            output.report_error(
                ErrorClass::Sync,
                &format!("Auto-sync worker failed during shutdown: {err}"),
            );
            exit_code = EXIT_ERROR;
        }
        match client.has_pending_push_changes() {
            Ok(true) => match rt.block_on(client.push()) {
                Ok(_) => {}
                Err(contextdb_core::Error::SyncPushUnconfirmed { detail }) => {
                    // Interrupted after send: the data may already have landed.
                    // Not a definitive failure, but the outcome is unknown, so
                    // exit with the distinct unconfirmed code (without
                    // downgrading a harder error) — a `push && shutdown` caller
                    // must not read this as a clean success. The next push
                    // reconciles idempotently.
                    // Indeterminate, not failed: a notice, with exit code 3
                    // carrying the "re-push is safe" signal.
                    output.report_notice(
                        ErrorClass::Sync,
                        &format!("Final sync push was interrupted before the hub acknowledged: {detail}. The data may already have landed; run `.sync push` on next start to reconcile."),
                    );
                    if exit_code == 0 {
                        exit_code = EXIT_INTERRUPTED_PUSH_UNCONFIRMED;
                    }
                }
                Err(contextdb_core::Error::SyncReplayOfAcceptedDelete { table, key }) => {
                    // The store already agrees: this exit-push re-offered a
                    // row the hub had already terminated by an accepted
                    // delete (the durable delete row itself has no local
                    // "arrived by sync" provenance surviving a fresh
                    // process, so the preflight above could not tell it
                    // apart from a genuine unpushed local change — see
                    // `Error::SyncReplayOfAcceptedDelete`'s doc comment).
                    // The hub's typed refusal proves both sides already
                    // converged on the delete, so this is benign, not a
                    // failure: a quiet notice, exit code left at whatever it
                    // already was (never downgraded to success, never
                    // bumped to a failure).
                    output.report_notice(
                        ErrorClass::Sync,
                        &format!(
                            "Final sync push re-offered {table} {key:?}, which the hub already \
                             converged on as deleted; nothing to do."
                        ),
                    );
                }
                Err(err) => {
                    output.report_error(
                        ErrorClass::of(&err),
                        &format!("Final sync push failed: {err}"),
                    );
                    exit_code = exit_code_for(&err);
                }
            },
            Ok(false) => {}
            Err(err) => {
                output.report_error(
                    ErrorClass::of(&err),
                    &format!("Final sync preflight failed: {err}"),
                );
                exit_code = exit_code_for(&err);
            }
        }
        rt.block_on(async {
            // Settle the startup probe first, then close the sync connection
            // and its endpoint gracefully so exit never aborts the transport
            // mid-flight.
            if let Some(handle) = probe_handle {
                let _ = handle.await;
            }
            client.shutdown().await;
            drop(client);
        });
    }

    if let Err(e) = db.close() {
        output.report_error(
            ErrorClass::of(&e),
            &format!("failed to close database: {e}"),
        );
        std::process::exit(EXIT_ERROR);
    }

    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}

fn parse_size_limit(value: &str) -> Result<usize, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("limit cannot be empty".to_string());
    }

    let split_at = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (digits, suffix) = trimmed.split_at(split_at);
    let base = digits
        .parse::<usize>()
        .map_err(|_| format!("invalid byte value '{trimmed}'"))?;
    let multiplier = match suffix.trim().to_ascii_uppercase().as_str() {
        "" => 1usize,
        "K" => 1024usize,
        "M" => 1024usize * 1024,
        "G" => 1024usize * 1024 * 1024,
        other => return Err(format!("unsupported memory suffix '{other}'")),
    };

    base.checked_mul(multiplier)
        .ok_or_else(|| format!("memory limit '{trimmed}' is too large"))
}
