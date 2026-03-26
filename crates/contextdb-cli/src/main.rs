use clap::Parser;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

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
}

fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();

    if let Some(ref _limit_str) = args.memory_limit {
        // Stub: flag is parsed but not applied.
        // A-MA1 fails because SHOW MEMORY_LIMIT still returns "none".
    }

    // If sync is configured, create the SyncPlugin before opening the DB.
    // Keep the rx end alive — a background task will consume it for debounced pushes.
    let (sync_plugin_arc, push_rx) = if args.tenant_id.is_some() {
        let (tx, rx) = tokio::sync::mpsc::channel::<()>(16);
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
            contextdb_engine::Database::open_memory_with_plugin(plugin.clone())
                .expect("failed to open memory database with plugin")
        } else {
            contextdb_engine::Database::open_memory()
        }
    } else if let Some(ref plugin) = sync_plugin_arc {
        match contextdb_engine::Database::open_with_plugin(
            std::path::Path::new(&args.path),
            plugin.clone(),
        ) {
            Ok(db) => db,
            Err(e) => {
                eprintln!("Error: failed to open database at '{}': {e}", args.path);
                std::process::exit(1);
            }
        }
    } else {
        match contextdb_engine::Database::open(std::path::Path::new(&args.path)) {
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
    let push_handle = if let (Some(rt_ref), Some(client), Some(mut rx)) = (rt, sync_client, push_rx)
    {
        let client_clone = Arc::clone(client);
        Some(rt_ref.spawn(async move {
            while rx.recv().await.is_some() {
                // Debounce: drain any additional signals within 500ms
                tokio::time::sleep(Duration::from_millis(500)).await;
                while rx.try_recv().is_ok() {}
                // Push
                if let Ok(result) = client_clone.push().await {
                    for conflict in &result.conflicts {
                        if let Some(reason) = &conflict.reason {
                            eprintln!("sync conflict: {}", reason);
                        }
                    }
                }
            }
        }))
    } else {
        None
    };

    let all_ok = repl::run(
        db.clone(),
        sync_client.map(|c| c.as_ref()),
        rt,
        sync_plugin_arc.as_deref(),
    );

    if let Err(e) = db.close() {
        eprintln!("Error: failed to close database: {e}");
        std::process::exit(1);
    }

    // Graceful shutdown: flush any unsent changes, then stop background task.
    if let Some((rt, client)) = rt_and_client {
        // Final push to flush pending changes (regardless of auto-sync setting)
        let _ = rt.block_on(client.push());
        // Stop background push task
        if let Some(ref plugin) = sync_plugin_arc {
            plugin.shutdown();
        }
        if let Some(handle) = push_handle {
            let _ = rt.block_on(handle);
        }
        rt.block_on(async {
            drop(client);
        });
    }

    if !all_ok {
        std::process::exit(1);
    }
}
