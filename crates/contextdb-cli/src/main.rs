use clap::Parser;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

mod auto_sync;
mod formatter;
mod repl;

#[derive(Parser)]
#[command(name = "contextdb-cli", version)]
struct Args {
    /// Database path (:memory: for in-memory)
    path: String,

    /// NATS URL for sync (WebSocket for edge)
    #[arg(
        long,
        env = "CONTEXTDB_NATS_URL",
        default_value = "ws://localhost:9222"
    )]
    nats_url: String,

    /// Tenant ID for sync
    #[arg(long, env = "CONTEXTDB_TENANT_ID")]
    tenant_id: Option<String>,

    /// Memory limit (e.g. 4G, 512M). Sets startup ceiling.
    #[arg(long, env = "CONTEXTDB_MEMORY_LIMIT")]
    memory_limit: Option<String>,

    /// Debounce interval for background auto-sync pushes.
    #[arg(long, env = "CONTEXTDB_SYNC_DEBOUNCE_MS", default_value_t = 500)]
    sync_debounce_ms: u64,
}

fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let accountant = args
        .memory_limit
        .as_ref()
        .map(|limit| parse_memory_limit(limit).map(contextdb_core::MemoryAccountant::with_budget))
        .transpose()
        .unwrap_or_else(|err| {
            eprintln!("Error: invalid --memory-limit: {err}");
            std::process::exit(1);
        })
        .map(Arc::new)
        .unwrap_or_else(|| Arc::new(contextdb_core::MemoryAccountant::no_limit()));

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

    debug!(path = %args.path, "opening database");
    let db = if args.path == ":memory:" {
        if let Some(ref plugin) = sync_plugin_arc {
            contextdb_engine::Database::open_memory_with_plugin_and_accountant(
                plugin.clone(),
                accountant.clone(),
            )
            .expect("failed to open memory database with plugin")
        } else {
            contextdb_engine::Database::open_memory_with_accountant(accountant.clone())
        }
    } else if let Some(ref plugin) = sync_plugin_arc {
        match contextdb_engine::Database::open_with_config(
            std::path::Path::new(&args.path),
            plugin.clone(),
            accountant.clone(),
        ) {
            Ok(db) => db,
            Err(e) => {
                eprintln!("Error: failed to open database at '{}': {e}", args.path);
                std::process::exit(1);
            }
        }
    } else {
        match contextdb_engine::Database::open_with_config(
            std::path::Path::new(&args.path),
            Arc::new(contextdb_engine::plugin::CorePlugin),
            accountant.clone(),
        ) {
            Ok(db) => db,
            Err(e) => {
                eprintln!("Error: failed to open database at '{}': {e}", args.path);
                std::process::exit(1);
            }
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
        let client = Arc::new(contextdb_server::SyncClient::new(
            db.clone(),
            &args.nats_url,
            tenant_id,
        ));
        (rt, client)
    });

    let (rt, sync_client) = match &rt_and_client {
        Some((rt, client)) => (Some(rt), Some(client)),
        None => (None, None),
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
                    let result = client.push().await.map_err(|err| err.to_string())?;
                    Ok(auto_sync::PushOutcome {
                        conflicts: result
                            .conflicts
                            .into_iter()
                            .filter_map(|conflict| conflict.reason)
                            .collect::<Vec<_>>(),
                        caught_up: client.push_watermark() >= plugin.pending_lsn(),
                    })
                }
            },
            |msg| eprintln!("{msg}"),
        )))
    } else {
        None
    };

    let mut all_ok = repl::run(
        db.clone(),
        sync_client.map(|c| c.as_ref()),
        rt,
        sync_plugin_arc.as_deref(),
    );

    // Graceful shutdown: stop background notifications, wait for any in-flight
    // auto-sync work to finish, then do one final flush before closing the DB.
    if let Some((rt, client)) = rt_and_client {
        if let Some(ref plugin) = sync_plugin_arc {
            plugin.shutdown();
        }
        if let Some(handle) = push_handle
            && let Err(err) = rt.block_on(handle)
        {
            eprintln!("Auto-sync worker failed during shutdown: {err}");
            all_ok = false;
        }
        match client.has_pending_push_changes() {
            Ok(true) => {
                if let Err(err) = rt.block_on(client.push()) {
                    eprintln!("Final sync push failed: {err}");
                    all_ok = false;
                }
            }
            Ok(false) => {}
            Err(err) => {
                eprintln!("Final sync preflight failed: {err}");
                all_ok = false;
            }
        }
        rt.block_on(async {
            drop(client);
        });
    }

    if let Err(e) = db.close() {
        eprintln!("Error: failed to close database: {e}");
        std::process::exit(1);
    }

    if !all_ok {
        std::process::exit(1);
    }
}

fn parse_memory_limit(value: &str) -> Result<usize, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("memory limit cannot be empty".to_string());
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
