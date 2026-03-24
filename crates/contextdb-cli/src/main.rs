use clap::Parser;
use std::sync::Arc;
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
}

fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();

    debug!(path = %args.path, "opening database");
    let db = if args.path == ":memory:" {
        contextdb_engine::Database::open_memory()
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

    // Single tokio runtime for the session — reused by all .sync commands.
    // Uses multi_thread with 1 worker so NATS background tasks (pings, heartbeats)
    // are polled even while the main thread blocks on readline.
    // Only created when sync is configured.
    let rt_and_client = args.tenant_id.as_ref().map(|tenant_id| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("failed to create tokio runtime");
        let client = contextdb_server::SyncClient::new(db.clone(), &args.nats_url, tenant_id);
        (rt, client)
    });

    let (rt, sync_client) = match &rt_and_client {
        Some((rt, client)) => (Some(rt), Some(client)),
        None => (None, None),
    };

    let all_ok = repl::run(db.clone(), sync_client, rt);

    if let Err(e) = db.close() {
        eprintln!("Error: failed to close database: {e}");
        std::process::exit(1);
    }

    // Graceful shutdown: drop SyncClient within the runtime so the internal
    // async_nats::Client can flush its buffer via the still-running worker thread.
    if let Some((rt, client)) = rt_and_client {
        rt.block_on(async {
            drop(client);
        });
    }

    if !all_ok {
        std::process::exit(1);
    }
}
