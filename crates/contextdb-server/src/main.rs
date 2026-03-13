use clap::Parser;
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_server::SyncServer;
use std::sync::Arc;

#[derive(Parser)]
struct Args {
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
    let db = Arc::new(Database::open_memory());
    let policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
    let server = SyncServer::new(db, &args.nats_url, &args.tenant_id, policies);
    server.run().await;
    Ok(())
}
