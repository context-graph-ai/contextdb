use clap::Parser;
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_server::SyncServer;
use contextdb_server::protocol::PROTOCOL_VERSION;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Parser)]
#[command(
    name = "contextdb-server",
    version = concat!(env!("CARGO_PKG_VERSION"), " protocol_version=4")
)]
struct Args {
    /// Database path (:memory: for in-memory)
    #[arg(long, env = "CONTEXTDB_DB_PATH", default_value = ":memory:")]
    db_path: String,
    #[arg(
        long,
        env = "CONTEXTDB_NATS_URL",
        default_value = "nats://localhost:4222"
    )]
    nats_url: String,
    #[arg(long, env = "CONTEXTDB_TENANT_ID")]
    tenant_id: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    tracing::info!(
        protocol_version = PROTOCOL_VERSION,
        "contextdb-server starting"
    );
    let db = if args.db_path == ":memory:" {
        Arc::new(Database::open_memory())
    } else {
        Arc::new(Database::open(std::path::Path::new(&args.db_path))?)
    };
    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);
    let server = SyncServer::new(db, &args.nats_url, &args.tenant_id, policies);
    let shutdown = Arc::new(AtomicBool::new(false));
    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        signal_shutdown.store(true, Ordering::SeqCst);
    });
    server.run_until(shutdown).await;
    server.db().close()?;
    Ok(())
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
